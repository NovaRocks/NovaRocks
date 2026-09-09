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
//! Core operator traits and blocking semantics.
//!
//! Responsibilities:
//! - Defines source/processor/sink execution contracts and blocked-reason signaling.
//! - Used by drivers to orchestrate cooperative operator execution steps.
//!
//! Key exported interfaces:
//! - Types: `BlockedReason`, `Operator`, `ProcessorOperator`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::io::FragmentEventSink;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::OperatorProfiles;
use crate::runtime::runtime_state::RuntimeState;
use arrow::datatypes::DataType;
use novarocks_types::SlotId;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq)]
/// The execution engine uses cooperative scheduling.
///
/// Operators are driven by a [`PipelineDriver`](crate::exec::pipeline::driver::PipelineDriver)
/// which repeatedly tries to move data from upstream to downstream.
/// When a driver cannot make progress without blocking, it records a [`BlockedReason`]
/// and yields.
pub enum BlockedReason {
    /// Upstream currently has no data available.
    InputEmpty,
    /// Downstream cannot accept more output at the moment.
    OutputFull,
    /// Blocked on a dependency object (e.g. build-side ready).
    Dependency(DependencyHandle),
}

/// Worker-frozen deadline that may invalidate one event-blocked decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverBlockDeadline {
    at: Instant,
    token: u64,
}

impl DriverBlockDeadline {
    pub(crate) fn new(at: Instant, token: u64) -> Self {
        Self { at, token }
    }

    pub(crate) fn at(self) -> Instant {
        self.at
    }

    pub(crate) fn token(self) -> u64 {
        self.token
    }
}

/// Base operator contract implemented by source/processor/sink operator implementations.
pub trait Operator: Send {
    fn name(&self) -> &str;

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let _ = tracker;
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        let _ = profiles;
    }

    fn prepare(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn bind_runtime_state(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }

    fn set_fragment_event_sink(&mut self, _sink: Arc<dyn FragmentEventSink>) {}

    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn cancel(&mut self) {
        // Default: nothing to cancel.
    }

    /// Handles a driver execution failure before the operator is closed.
    ///
    /// The default preserves the existing cleanup behavior for operators that do not distinguish
    /// execution failure from cancellation.
    fn on_driver_failure(&mut self) {
        self.cancel();
    }

    fn is_finished(&self) -> bool {
        false
    }

    fn pending_finish(&self) -> bool {
        false
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        None
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        None
    }
}

/// What an operator's unfinished `set_finishing` is waiting for.
///
/// The driver asks this one question for two different decisions, and the two
/// need different answers, which is why this is not a boolean:
///
/// * whether the operator may be latched as finished with `set_finishing` —
///   only [`Self::Complete`] may;
/// * whether giving it another turn right now would accomplish anything —
///   only [`Self::OwedOutput`] would.
///
/// Collapsing the two deadlocks the driver in one direction or spins it in the
/// other. Reporting owed output as "cannot progress" parks a driver forever
/// with the payload still in hand — measured on a real 1FE+3BE cluster as a
/// distributed `SELECT` whose one row-carrying producer never sent its end of
/// stream. Reporting a wait for an external event as "can progress" turns the
/// park into a hot spin over an obstacle the operator cannot clear.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum FinishingWait {
    /// Finishing is done for this operator; the driver may latch it.
    #[default]
    Complete,
    /// Output the operator still owes and can push on its next turn: a payload
    /// it has buffered, or an end-of-stream marker it has not sent. The driver
    /// must give it that turn.
    OwedOutput,
    /// An event only something else can produce — an exchange sink's outbound
    /// edge has not been granted send permission, for one. Another turn now
    /// would do nothing, so the driver parks and its blocked-driver poller
    /// re-asks.
    ExternalEvent,
}

impl FinishingWait {
    /// Whether finishing is still unfinished, whatever it is waiting for.
    pub const fn is_pending(self) -> bool {
        !matches!(self, Self::Complete)
    }

    /// Whether another turn right now would move this operator forward.
    pub const fn can_progress(self) -> bool {
        matches!(self, Self::OwedOutput)
    }
}

/// Extended operator contract for processor stages with push/pull semantics.
pub trait ProcessorOperator: Operator {
    fn need_input(&self) -> bool;

    /// Non-blocking admission for the exact chunk currently retained on the
    /// upstream edge.
    ///
    /// Most operators use their ordinary `need_input` decision. A bounded
    /// terminal sink may reserve byte credit here before the driver transfers
    /// ownership to `push_chunk`.
    fn can_accept_input(&self, _chunk: &Chunk) -> Result<bool, String> {
        Ok(self.need_input())
    }

    fn has_output(&self) -> bool;

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String>;

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String>;

    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String>;

    /// What `set_finishing` could not complete, and whether another turn
    /// would complete it.
    ///
    /// Default [`FinishingWait::Complete`]: for almost every operator,
    /// finishing completes in the one call the driver makes. An operator that
    /// can be legitimately unable to finish yet reports which of the two
    /// reasons it is, because the driver acts on them differently — see
    /// [`FinishingWait`].
    fn finishing_wait(&self) -> FinishingWait {
        FinishingWait::Complete
    }

    /// Whether this operator can consume the given column in its current
    /// physical encoding WITHOUT hydration. Default: false - the driver
    /// hydrates every encoded column before `push_chunk`. C1-C4 override
    /// this per slot to keep specific dictionary columns encoded on their
    /// fast path.
    fn accepts_encoded_column(&self, _slot_id: SlotId, _data_type: &DataType) -> bool {
        false
    }

    /// Dependency that must be ready before the operator can make progress.
    /// This is used for build-side readiness (join, runtime filters, etc.).
    fn precondition_dependency(&self) -> Option<DependencyHandle> {
        None
    }

    /// Observable for source-side readiness (has_output becomes true).
    ///
    /// The observable identity is part of the operator's scheduling contract:
    /// while the operator is not finished, every `Some` returned for this
    /// readiness direction must be the same `Arc` identity. Composite operators
    /// must therefore expose an operator-owned observable and forward every
    /// underlying readiness transition to it. A callback used for forwarding
    /// must retain the composite observable weakly so an underlying observable
    /// cannot extend the composite operator's lifetime.
    fn source_observable(&self) -> Option<Arc<Observable>> {
        None
    }

    /// Arm or return the deadline for the current confirmed source-idle block.
    /// Called only by the driver worker after `has_output` returned false.
    fn source_block_deadline(&self) -> Option<DriverBlockDeadline> {
        None
    }

    /// Observable for sink-side readiness (need_input becomes true).
    ///
    /// The same lifetime-stable identity and weak-forwarding requirements as
    /// [`ProcessorOperator::source_observable`] apply to this direction.
    fn sink_observable(&self) -> Option<Arc<Observable>> {
        None
    }
}

/// Forwards readiness transitions without retaining the target observable.
///
/// Composite operators use this once during construction to keep their public
/// observable identity stable while covering every child readiness source.
pub(crate) fn forward_observable(source: &Arc<Observable>, target: &Arc<Observable>) {
    if Arc::ptr_eq(source, target) {
        return;
    }
    let target = Arc::downgrade(target);
    source.add_observer(Arc::new(move || {
        if let Some(target) = target.upgrade() {
            target.notify_observers();
        }
    }));
}

/// Hydrate a chunk for delivery to `downstream`, keeping only the columns the
/// operator declares it can consume encoded. Default operators accept nothing
/// encoded, so this hydrates everything (C0 behavior).
pub(crate) fn hydrate_for_downstream(
    chunk: &Chunk,
    downstream: &dyn ProcessorOperator,
) -> Result<Chunk, String> {
    crate::exec::chunk::hydrate_dictionary_columns_except(chunk, |slot_id, dt| {
        downstream.accepts_encoded_column(slot_id, dt)
    })
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct DictionaryCarrierStats {
    pub input_rows: i64,
    pub input_columns: i64,
    pub kept_rows: i64,
    pub kept_columns: i64,
    pub hydrated_rows: i64,
    pub hydrated_columns: i64,
    pub unsupported_columns: i64,
}

impl DictionaryCarrierStats {
    pub fn has_input(self) -> bool {
        self.input_columns > 0
    }
}

pub(crate) fn dictionary_carrier_stats(
    chunk: &Chunk,
    downstream: &dyn ProcessorOperator,
) -> DictionaryCarrierStats {
    let rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);
    let mut stats = DictionaryCarrierStats::default();

    for slot in chunk.chunk_schema().slots() {
        let data_type = slot.data_type();
        if !matches!(data_type, arrow::datatypes::DataType::Dictionary(_, _)) {
            continue;
        }

        stats.input_columns = stats.input_columns.saturating_add(1);
        stats.input_rows = stats.input_rows.saturating_add(rows);

        if downstream.accepts_encoded_column(slot.slot_id(), data_type) {
            stats.kept_columns = stats.kept_columns.saturating_add(1);
            stats.kept_rows = stats.kept_rows.saturating_add(rows);
        } else {
            stats.hydrated_columns = stats.hydrated_columns.saturating_add(1);
            stats.hydrated_rows = stats.hydrated_rows.saturating_add(rows);
            stats.unsupported_columns = stats.unsupported_columns.saturating_add(1);
        }
    }

    stats
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        Chunk, Operator, ProcessorOperator, dictionary_carrier_stats, forward_observable,
        hydrate_for_downstream,
    };
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::pipeline::schedule::observer::Observable;
    use crate::runtime::runtime_state::RuntimeState;
    use arrow::array::{Array, ArrayRef, DictionaryArray, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    struct StubOp;

    impl Operator for StubOp {
        fn name(&self) -> &str {
            "stub"
        }
    }

    impl ProcessorOperator for StubOp {
        fn need_input(&self) -> bool {
            false
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
            Ok(())
        }
    }

    struct KeepSlot1Op;

    impl Operator for KeepSlot1Op {
        fn name(&self) -> &str {
            "keep-slot-1"
        }
    }

    impl ProcessorOperator for KeepSlot1Op {
        fn need_input(&self) -> bool {
            false
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
            Ok(())
        }

        fn accepts_encoded_column(&self, slot_id: SlotId, data_type: &DataType) -> bool {
            slot_id == SlotId::new(1)
                && matches!(
                    data_type,
                    DataType::Dictionary(key_type, value_type)
                        if key_type.as_ref() == &DataType::Int32
                            && value_type.as_ref() == &DataType::Utf8
                )
        }
    }

    fn dict_utf8_with_nulls_and_empty() -> ArrayRef {
        Arc::new(
            vec![Some("PAID"), None, Some(""), Some("NEW")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        )
    }

    fn two_dictionary_column_chunk() -> Chunk {
        let slot1 = SlotId::new(1);
        let slot2 = SlotId::new(2);
        let column1 = dict_utf8_with_nulls_and_empty();
        let column2 = dict_utf8_with_nulls_and_empty();
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::new_with_field(
                    slot1,
                    Field::new("status_1", column1.data_type().clone(), true),
                    None,
                    None,
                ),
                ChunkSlotSchema::new_with_field(
                    slot2,
                    Field::new("status_2", column2.data_type().clone(), true),
                    None,
                    None,
                ),
            ])
            .expect("chunk schema"),
        );
        let batch = RecordBatch::try_new(chunk_schema.arrow_schema_ref(), vec![column1, column2])
            .expect("record batch");
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk")
    }

    fn assert_utf8_values(column: &ArrayRef) {
        let values = column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(values.value(0), "PAID");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "");
        assert_eq!(values.value(3), "NEW");
    }

    #[test]
    fn processor_operator_rejects_physical_encodings_by_default() {
        let op = StubOp;
        let slot_id = SlotId::new(1);
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        assert!(!op.accepts_encoded_column(slot_id, &dictionary_type));
        assert!(!op.accepts_encoded_column(slot_id, &DataType::Utf8));
    }

    #[test]
    fn hydrate_for_downstream_hydrates_all_dictionary_columns_by_default() {
        let chunk = two_dictionary_column_chunk();
        let op = StubOp;

        let hydrated = hydrate_for_downstream(&chunk, &op).expect("hydrate for downstream");

        assert_eq!(hydrated.columns()[0].data_type(), &DataType::Utf8);
        assert_eq!(hydrated.columns()[1].data_type(), &DataType::Utf8);
        assert_eq!(
            hydrated
                .chunk_schema()
                .slot(SlotId::new(1))
                .expect("slot 1")
                .data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            hydrated
                .chunk_schema()
                .slot(SlotId::new(2))
                .expect("slot 2")
                .data_type(),
            &DataType::Utf8
        );
        assert_utf8_values(&hydrated.columns()[0]);
        assert_utf8_values(&hydrated.columns()[1]);
    }

    #[test]
    fn hydrate_for_downstream_keeps_only_declared_encoded_slots() {
        let chunk = two_dictionary_column_chunk();
        let op = KeepSlot1Op;

        let hydrated = hydrate_for_downstream(&chunk, &op).expect("hydrate for downstream");

        assert!(matches!(
            hydrated.columns()[0].data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
        ));
        assert!(matches!(
            hydrated
                .chunk_schema()
                .slot(SlotId::new(1))
                .expect("slot 1")
                .data_type(),
            DataType::Dictionary(key_type, value_type)
                if key_type.as_ref() == &DataType::Int32 && value_type.as_ref() == &DataType::Utf8
        ));
        assert_eq!(hydrated.columns()[1].data_type(), &DataType::Utf8);
        assert_eq!(
            hydrated
                .chunk_schema()
                .slot(SlotId::new(2))
                .expect("slot 2")
                .data_type(),
            &DataType::Utf8
        );
        assert_utf8_values(&hydrated.columns()[1]);
    }

    #[test]
    fn dictionary_carrier_stats_hydrates_all_dictionary_columns_by_default() {
        let chunk = two_dictionary_column_chunk();
        let op = StubOp;

        let stats = dictionary_carrier_stats(&chunk, &op);

        assert_eq!(stats.input_rows, 8);
        assert_eq!(stats.input_columns, 2);
        assert_eq!(stats.kept_rows, 0);
        assert_eq!(stats.kept_columns, 0);
        assert_eq!(stats.hydrated_rows, 8);
        assert_eq!(stats.hydrated_columns, 2);
        assert_eq!(stats.unsupported_columns, 2);
    }

    #[test]
    fn dictionary_carrier_stats_keeps_declared_encoded_slot() {
        let chunk = two_dictionary_column_chunk();
        let op = KeepSlot1Op;

        let stats = dictionary_carrier_stats(&chunk, &op);

        assert_eq!(stats.input_rows, 8);
        assert_eq!(stats.input_columns, 2);
        assert_eq!(stats.kept_rows, 4);
        assert_eq!(stats.kept_columns, 1);
        assert_eq!(stats.hydrated_rows, 4);
        assert_eq!(stats.hydrated_columns, 1);
        assert_eq!(stats.unsupported_columns, 1);
    }

    #[test]
    fn observable_forwarding_does_not_retain_the_target() {
        let source = Arc::new(Observable::new());
        let target = Arc::new(Observable::new());
        let target_weak = Arc::downgrade(&target);
        forward_observable(&source, &target);

        drop(target);
        source.notify_observers();

        assert!(target_weak.upgrade().is_none());
    }
}
