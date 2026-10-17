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
//! Split stream sink for partitioned exchange output.
//!
//! Responsibilities:
//! - Splits incoming chunks into disjoint per-destination subsets based on split expressions.
//! - Sends split chunks to dedicated downstream data stream sinks.
//! - Preserves sink completion signaling.

use crate::runtime::fragment::ExecutionResult;

use std::sync::Arc;

use crate::runtime::fragment::io::exchange_edge::ExchangeEdgeGates;

use arrow::array::{Array, BooleanArray};
#[cfg(test)]
use arrow::compute::filter_record_batch;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::pipeline::operator::{
    FinishingWait, Operator, ProcessorOperator, forward_observable,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::io::ExchangeFrameTransmitter;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::OperatorProfiles;
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::UniqueId;

use super::DataStreamSinkFactory;
use crate::exec::fragment::sink::DataStreamSinkFactoryInput;

struct InnerSinkSpec {
    factory: DataStreamSinkFactory,
}

/// Factory for split stream sinks that partition rows to multiple remote channels.
pub struct SplitDataStreamSinkFactory {
    name: String,
    init_error: Option<String>,
    split_arena: Arc<ExprArena>,
    split_exprs: Vec<ExprId>,
    fanout: bool,
    sinks: Vec<InnerSinkSpec>,
}

impl SplitDataStreamSinkFactory {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sinks: Vec<DataStreamSinkFactoryInput>,
        fragment_instance_id: UniqueId,
        sender_id: Option<i32>,
        partition_arena: ExprArena,
        plan_node_id: i32,
        split_arena: Arc<ExprArena>,
        split_exprs: Vec<ExprId>,
        fanout: bool,
        transmitter: Arc<dyn ExchangeFrameTransmitter>,
    ) -> Self {
        let name = if plan_node_id >= 0 {
            format!("SPLIT_DATA_STREAM_SINK (id={plan_node_id})")
        } else {
            "SPLIT_DATA_STREAM_SINK".to_string()
        };

        let mut init_error = None;
        let mut sinks_out = Vec::new();

        if sinks.is_empty() {
            init_error = Some("SPLIT_DATA_STREAM_SINK requires at least one sink".to_string());
        } else if split_exprs.len() != sinks.len() {
            init_error = Some(format!(
                "SPLIT_DATA_STREAM_SINK: split_exprs size {} != sinks size {}",
                split_exprs.len(),
                sinks.len()
            ));
        } else {
            for sink in sinks {
                sinks_out.push(InnerSinkSpec {
                    factory: DataStreamSinkFactory::new(
                        sink,
                        fragment_instance_id,
                        sender_id,
                        plan_node_id,
                        partition_arena.clone(),
                        Arc::clone(&transmitter),
                    ),
                });
            }
        }

        Self {
            name,
            init_error,
            split_arena,
            split_exprs,
            fanout,
            sinks: sinks_out,
        }
    }

    /// Grants the inner sinks the edge gates their destinations are bound by.
    ///
    /// A composite sink's branches all belong to the same task, so one gate
    /// set governs all of them; forwarding here keeps the send path's
    /// permission check in exactly one place.
    /// Whether every branch is bound by a gate set.
    #[cfg(test)]
    pub(crate) fn is_edge_gated(&self) -> bool {
        !self.sinks.is_empty() && self.sinks.iter().all(|spec| spec.factory.is_edge_gated())
    }

    pub fn with_edge_gates(mut self, gates: Arc<ExchangeEdgeGates>) -> Self {
        self.sinks = self
            .sinks
            .into_iter()
            .map(|mut spec| {
                spec.factory = spec.factory.with_edge_gates(Arc::clone(&gates));
                spec
            })
            .collect();
        self
    }
}

impl OperatorFactory for SplitDataStreamSinkFactory {
    #[cfg(test)]
    fn is_edge_gated(&self) -> bool {
        Self::is_edge_gated(self)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let mut sinks = Vec::with_capacity(self.sinks.len());
        for spec in &self.sinks {
            sinks.push(InnerSinkRuntime {
                op: spec.factory.create(dop, driver_id),
            });
        }

        Box::new(SplitDataStreamSinkOperator::new(
            self.name.clone(),
            self.init_error.clone(),
            Arc::clone(&self.split_arena),
            self.split_exprs.clone(),
            self.fanout,
            sinks,
        ))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct InnerSinkRuntime {
    op: Box<dyn Operator>,
}

struct SplitDataStreamSinkOperator {
    name: String,
    init_error: Option<String>,
    split_arena: Arc<ExprArena>,
    split_exprs: Vec<ExprId>,
    fanout: bool,
    sinks: Vec<InnerSinkRuntime>,
    finishing: bool,
    sink_observable: Arc<Observable>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
}

impl SplitDataStreamSinkOperator {
    fn new(
        name: String,
        init_error: Option<String>,
        split_arena: Arc<ExprArena>,
        split_exprs: Vec<ExprId>,
        fanout: bool,
        sinks: Vec<InnerSinkRuntime>,
    ) -> Self {
        let sink_observable = Arc::new(Observable::new());
        for sink in &sinks {
            if let Some(observable) = sink
                .op
                .as_processor_ref()
                .and_then(ProcessorOperator::sink_observable)
            {
                forward_observable(&observable, &sink_observable);
            }
        }
        Self {
            name,
            init_error,
            split_arena,
            split_exprs,
            fanout,
            sinks,
            finishing: false,
            sink_observable,
            construction: None,
        }
    }
}

impl Operator for SplitDataStreamSinkOperator {
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        for sink in &mut self.sinks {
            sink.op.set_construction_factory(factory.clone());
        }
        self.construction = Some(factory);
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        for sink in &mut self.sinks {
            sink.op.set_mem_tracker(Arc::clone(&tracker));
        }
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        for sink in &mut self.sinks {
            sink.op.set_profiles(profiles.clone());
        }
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        for sink in &mut self.sinks {
            sink.op.bind_runtime_state(state)?;
        }
        Ok(())
    }

    fn activate(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        for sink in &mut self.sinks {
            sink.op.activate(state)?;
        }
        Ok(())
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        for sink in &mut self.sinks {
            sink.op.prepare()?;
        }
        Ok(())
    }

    fn close(&mut self) -> ExecutionResult<()> {
        for sink in &mut self.sinks {
            sink.op.close()?;
        }
        Ok(())
    }

    fn cancel(&mut self) {
        for sink in &mut self.sinks {
            sink.op.cancel();
        }
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finishing && self.sinks.iter().all(|sink| sink.op.is_finished())
    }
}

impl ProcessorOperator for SplitDataStreamSinkOperator {
    /// The aggregate of what this sink's branches are still waiting for.
    ///
    /// The same contract [`MultiCastDataStreamSinkOperator::finishing_wait`]
    /// carries, for the same reason: without it the default `Complete`
    /// applies, the driver latches finishing after one call, and a branch that
    /// was not ready at that call never sends its end-of-stream. `OwedOutput`
    /// outranks `ExternalEvent` because branches finish independently.
    fn finishing_wait(&self) -> FinishingWait {
        if !self.finishing {
            return FinishingWait::Complete;
        }
        let mut wait = FinishingWait::Complete;
        for sink in &self.sinks {
            let Some(inner) = sink.op.as_processor_ref() else {
                continue;
            };
            match inner.finishing_wait() {
                FinishingWait::OwedOutput => return FinishingWait::OwedOutput,
                FinishingWait::ExternalEvent => wait = FinishingWait::ExternalEvent,
                FinishingWait::Complete => {}
            }
        }
        wait
    }

    fn need_input(&self) -> bool {
        if self.is_finished() || self.finishing {
            return false;
        }
        for sink in &self.sinks {
            let Some(inner) = sink.op.as_processor_ref() else {
                return false;
            };
            if !inner.need_input() {
                return false;
            }
        }
        true
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if let Some(err) = self.init_error.as_ref() {
            return Err(err.clone().into());
        }
        if self.is_finished() || self.finishing {
            return Ok(());
        }
        if chunk.is_empty() || self.sinks.is_empty() {
            return Ok(());
        }

        let factory = self
            .construction
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let control = super::compiled_expression::RuntimeKernelControl::new(state.error_state());
        let split_chunks = split_chunk_by_exprs_with(
            &self.split_exprs,
            chunk,
            self.fanout,
            &mut |expr, input| {
                let owner = factory.begin_input_construction(input)?;
                crate::exec::expr::ExprEvaluationContext::new(
                    &self.split_arena,
                    owner.as_ref(),
                    &control,
                )
                .eval(expr, input)
            },
            &mut |input, mask| {
                let owner = factory.begin_input_construction(input)?;
                input.filter_for_evaluation(mask, owner.as_ref())
            },
        )?;
        if split_chunks.len() != self.sinks.len() {
            return Err(format!(
                "split chunk output size {} != sink size {}",
                split_chunks.len(),
                self.sinks.len()
            )
            .into());
        }
        for (sink, part) in self.sinks.iter_mut().zip(split_chunks.into_iter()) {
            let Some(part) = part else {
                continue;
            };
            let inner = sink
                .op
                .as_processor_mut()
                .ok_or_else(|| "inner data stream op missing processor operator".to_string())?;
            inner.push_chunk(state, part)?;
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        Ok(None)
    }

    /// Drives every branch's finishing, on this call and on every retry.
    ///
    /// See [`MultiCastDataStreamSinkOperator::set_finishing`]: a branch whose
    /// edge is still closed or which still holds a parked payload needs the
    /// later turns, and returning early once `finishing` was set swallowed
    /// them.
    fn set_finishing(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        if let Some(err) = self.init_error.as_ref() {
            return Err(err.clone().into());
        }
        let first_call = !self.finishing;
        self.finishing = true;
        for sink in &mut self.sinks {
            let inner = sink
                .op
                .as_processor_mut()
                .ok_or_else(|| "inner data stream op missing processor operator".to_string())?;
            if first_call || inner.finishing_wait().is_pending() {
                inner.set_finishing(state)?;
            }
        }
        Ok(())
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        if self.is_finished() {
            return None;
        }
        Some(Arc::clone(&self.sink_observable))
    }
}

#[cfg(test)]
fn split_chunk_by_exprs<E: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &E,
    exprs: &[ExprId],
    chunk: Chunk,
    fanout: bool,
) -> Result<Vec<Option<Chunk>>, E::Error> {
    if chunk.chunk_schema().is_typed() {
        return Err(E::Error::from(
            "reference split evaluation requires an untyped input".to_owned(),
        ));
    }
    split_chunk_by_exprs_with(
        exprs,
        chunk,
        fanout,
        &mut |expr, input| arena.eval(expr, input),
        &mut |input, mask| {
            let filtered = filter_record_batch(input.record_batch(), mask).map_err(|error| {
                E::Error::from(format!("split sink filter chunk failed: {error}"))
            })?;
            Chunk::try_new_like(filtered, input).map_err(Into::into)
        },
    )
}

fn split_chunk_by_exprs_with<E: From<String>>(
    split_exprs: &[ExprId],
    chunk: Chunk,
    fanout: bool,
    evaluate: &mut dyn FnMut(ExprId, &Chunk) -> Result<arrow::array::ArrayRef, E>,
    filter_input: &mut dyn FnMut(&Chunk, &BooleanArray) -> Result<Chunk, E>,
) -> Result<Vec<Option<Chunk>>, E> {
    if split_exprs.is_empty() {
        return Ok(vec![]);
    }

    if fanout {
        // A row-mutation route is a filter, not an exclusive partition.  In
        // particular, a logical Replace must reach both the delete and
        // replacement-data routes.
        return split_exprs
            .iter()
            .map(|expr| {
                eval_split_mask(evaluate, *expr, &chunk)
                    .and_then(|mask| filter_chunk_by_mask(&chunk, &mask, filter_input))
            })
            .collect();
    }

    let mut out = vec![None; split_exprs.len()];
    let mut remaining = chunk;

    for idx in (1..split_exprs.len()).rev() {
        if remaining.is_empty() {
            break;
        }
        let mask = eval_split_mask(evaluate, split_exprs[idx], &remaining)?;
        let matched = mask.iter().filter(|flag| **flag).count();
        if matched == 0 {
            continue;
        }
        if matched == remaining.len() {
            out[idx] = Some(remaining);
            return Ok(out);
        }

        out[idx] = filter_chunk_by_mask(&remaining, &mask, filter_input)?;
        let remaining_mask = mask.into_iter().map(|flag| !flag).collect::<Vec<_>>();
        remaining = filter_chunk_by_mask(&remaining, &remaining_mask, filter_input)?
            .ok_or_else(|| "split sink produced empty remaining chunk unexpectedly".to_string())?;
    }

    if !remaining.is_empty() {
        let mask = eval_split_mask(evaluate, split_exprs[0], &remaining)?;
        out[0] = filter_chunk_by_mask(&remaining, &mask, filter_input)?;
    }
    Ok(out)
}

fn eval_split_mask<E: From<String>>(
    evaluate: &mut dyn FnMut(ExprId, &Chunk) -> Result<arrow::array::ArrayRef, E>,
    expr_id: ExprId,
    chunk: &Chunk,
) -> Result<Vec<bool>, E> {
    let predicate_array = evaluate(expr_id, chunk)?;
    let mask = predicate_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "split expr must return BOOLEAN".to_string())?;
    if mask.len() != chunk.len() {
        return Err(format!(
            "split expr result length {} != chunk length {}",
            mask.len(),
            chunk.len()
        )
        .into());
    }

    let mut out = Vec::with_capacity(mask.len());
    for i in 0..mask.len() {
        out.push(mask.is_valid(i) && mask.value(i));
    }
    Ok(out)
}

fn filter_chunk_by_mask<E: From<String>>(
    chunk: &Chunk,
    mask: &[bool],
    filter_input: &mut dyn FnMut(&Chunk, &BooleanArray) -> Result<Chunk, E>,
) -> Result<Option<Chunk>, E> {
    if mask.len() != chunk.len() {
        return Err(format!(
            "filter mask length {} != chunk length {}",
            mask.len(),
            chunk.len()
        )
        .into());
    }

    let selected = mask.iter().filter(|flag| **flag).count();
    if selected == 0 {
        return Ok(None);
    }
    if selected == chunk.len() {
        return Ok(Some(chunk.clone()));
    }

    let predicate = BooleanArray::from(mask.to_vec());
    Ok(Some(filter_input(chunk, &predicate)?))
}

#[cfg(test)]
mod tests {
    use crate::runtime::fragment::ExecutionResult;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use arrow::array::{BooleanArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::split_chunk_by_exprs;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::pipeline::operator::{FinishingWait, Operator, ProcessorOperator};
    use crate::exec::pipeline::schedule::observer::Observable;
    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_types::SlotId;

    use super::{InnerSinkRuntime, SplitDataStreamSinkOperator};

    struct PendingFinishSink {
        name: String,
        finishing: bool,
        finished: Arc<AtomicBool>,
        observable: Arc<Observable>,
    }

    impl PendingFinishSink {
        fn new(name: &str, finished: Arc<AtomicBool>) -> Self {
            Self {
                name: name.to_string(),
                finishing: false,
                finished,
                observable: Arc::new(Observable::new()),
            }
        }
    }

    impl Operator for PendingFinishSink {
        fn name(&self) -> &str {
            &self.name
        }

        fn is_finished(&self) -> bool {
            self.finished.load(Ordering::SeqCst)
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for PendingFinishSink {
        fn need_input(&self) -> bool {
            !self.finishing && !self.is_finished()
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> ExecutionResult<()> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
            self.finishing = true;
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            (!self.is_finished()).then(|| Arc::clone(&self.observable))
        }
    }

    #[test]
    fn split_sink_waits_for_inner_sinks_to_finish() {
        let first_done = Arc::new(AtomicBool::new(false));
        let second_done = Arc::new(AtomicBool::new(false));
        let mut op = SplitDataStreamSinkOperator::new(
            "SPLIT_DATA_STREAM_SINK(test)".to_string(),
            None,
            Arc::new(ExprArena::default()),
            Vec::new(),
            false,
            vec![
                InnerSinkRuntime {
                    op: Box::new(PendingFinishSink::new("first", Arc::clone(&first_done))),
                },
                InnerSinkRuntime {
                    op: Box::new(PendingFinishSink::new("second", Arc::clone(&second_done))),
                },
            ],
        );

        let state = RuntimeState::default();
        op.set_finishing(&state).expect("set finishing");
        assert!(!op.is_finished(), "wrapper must wait for async inner sinks");
        assert!(op.sink_observable().is_some());

        first_done.store(true, Ordering::SeqCst);
        assert!(!op.is_finished(), "wrapper must wait for every inner sink");

        second_done.store(true, Ordering::SeqCst);
        assert!(op.is_finished());
        assert!(op.sink_observable().is_none());
    }

    #[test]
    fn split_sink_uses_one_identity_and_forwards_non_first_inner_wakes() {
        let first_done = Arc::new(AtomicBool::new(false));
        let second_done = Arc::new(AtomicBool::new(false));
        let first = PendingFinishSink::new("first", first_done);
        let second = PendingFinishSink::new("second", second_done);
        let second_observable = Arc::clone(&second.observable);
        let op = SplitDataStreamSinkOperator::new(
            "SPLIT_DATA_STREAM_SINK(test)".to_string(),
            None,
            Arc::new(ExprArena::default()),
            Vec::new(),
            false,
            vec![
                InnerSinkRuntime {
                    op: Box::new(first),
                },
                InnerSinkRuntime {
                    op: Box::new(second),
                },
            ],
        );
        let first_identity = op.sink_observable().expect("stable sink observable");
        let generation = first_identity.generation();

        second_observable.notify_observers();

        let second_identity = op.sink_observable().expect("stable sink observable");
        assert!(Arc::ptr_eq(&first_identity, &second_identity));
        assert_eq!(second_identity.generation(), generation + 1);
    }

    #[test]
    fn split_sink_applies_the_first_branch_predicate_instead_of_using_it_as_fallback() {
        let predicate_slot = SlotId::new(1);
        let value_slot = SlotId::new(2);
        let schema = Arc::new(Schema::new(vec![
            Field::new("selected", DataType::Boolean, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )
        .expect("input batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            schema.as_ref(),
            &[predicate_slot, value_slot],
        )
        .expect("input chunk schema");
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);

        let mut arena = ExprArena::default();
        let predicate = arena.push_typed(ExprNode::SlotId(predicate_slot), DataType::Boolean);
        let split = split_chunk_by_exprs(&arena, &[predicate], chunk, false).expect("split chunk");
        let selected = split[0].as_ref().expect("matching first branch rows");

        assert_eq!(selected.len(), 1);
        let values = selected
            .record_batch()
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("value column");
        assert_eq!(values.values(), &[10]);
    }

    /// Inner sink that seals only once its permission arrives, the way an
    /// exchange sink whose outbound edge is still closed behaves.
    struct GatedFinishSink {
        name: String,
        permitted: Arc<AtomicBool>,
        finishing: bool,
        sealed: Arc<AtomicBool>,
        observable: Arc<Observable>,
    }

    impl GatedFinishSink {
        fn new(name: &str, permitted: Arc<AtomicBool>, sealed: Arc<AtomicBool>) -> Self {
            Self {
                name: name.to_string(),
                permitted,
                finishing: false,
                sealed,
                observable: Arc::new(Observable::new()),
            }
        }
    }

    impl Operator for GatedFinishSink {
        fn name(&self) -> &str {
            &self.name
        }

        fn is_finished(&self) -> bool {
            self.sealed.load(Ordering::SeqCst)
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for GatedFinishSink {
        fn finishing_wait(&self) -> FinishingWait {
            if !self.finishing {
                return FinishingWait::Complete;
            }
            if !self.permitted.load(Ordering::SeqCst) {
                return FinishingWait::ExternalEvent;
            }
            if self.sealed.load(Ordering::SeqCst) {
                FinishingWait::Complete
            } else {
                FinishingWait::OwedOutput
            }
        }

        fn need_input(&self) -> bool {
            !self.finishing && !self.is_finished()
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> ExecutionResult<()> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
            self.finishing = true;
            if self.permitted.load(Ordering::SeqCst) {
                self.sealed.store(true, Ordering::SeqCst);
            }
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            (!self.is_finished()).then(|| Arc::clone(&self.observable))
        }
    }

    #[test]
    fn a_branch_that_could_not_seal_yet_is_driven_again() {
        // The same defect the multi-cast wrapper had: no finishing wait was
        // reported and the branches were never re-entered, so a branch that
        // could not seal on the first call never sent its end-of-stream and
        // its receiver waited for a sender that never sealed.
        let open = Arc::new(AtomicBool::new(true));
        let closed = Arc::new(AtomicBool::new(false));
        let open_sealed = Arc::new(AtomicBool::new(false));
        let closed_sealed = Arc::new(AtomicBool::new(false));
        let mut op = SplitDataStreamSinkOperator::new(
            "SPLIT_DATA_STREAM_SINK(test)".to_string(),
            None,
            Arc::new(ExprArena::default()),
            Vec::new(),
            false,
            vec![
                InnerSinkRuntime {
                    op: Box::new(GatedFinishSink::new(
                        "open",
                        Arc::clone(&open),
                        Arc::clone(&open_sealed),
                    )),
                },
                InnerSinkRuntime {
                    op: Box::new(GatedFinishSink::new(
                        "closed",
                        Arc::clone(&closed),
                        Arc::clone(&closed_sealed),
                    )),
                },
            ],
        );

        let state = RuntimeState::default();
        op.set_finishing(&state).expect("set finishing");
        assert!(open_sealed.load(Ordering::SeqCst));
        assert!(!closed_sealed.load(Ordering::SeqCst));
        assert!(
            op.finishing_wait().is_pending(),
            "the wrapper must ask the driver for another turn while a branch waits"
        );

        closed.store(true, Ordering::SeqCst);
        op.set_finishing(&state).expect("set finishing again");
        assert!(
            closed_sealed.load(Ordering::SeqCst),
            "the retry must reach the branch that was not ready before"
        );
        assert!(!op.finishing_wait().is_pending());
        assert!(op.is_finished());
    }
    fn typed_split_fixture() -> (ExprArena, Vec<crate::exec::expr::ExprId>, Chunk) {
        use crate::exec::chunk::ChunkSlotSchema;
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
        let slot = SlotId::new(91);
        let ty = FunctionValueType::new(DataType::Boolean, true);
        let field = Arc::new(Field::new("flag", DataType::Boolean, true));
        let schema = Arc::new(
            ChunkSchema::try_new_typed(
                vec![
                    ChunkSlotSchema::try_new_typed(
                        slot,
                        field,
                        ty.typed_binding().clone(),
                        None,
                        ValueTypeStorageLifetime::pure(),
                    )
                    .unwrap(),
                ],
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        );
        let input = Chunk::try_new_with_chunk_schema(
            RecordBatch::try_new(
                schema.arrow_schema_ref(),
                vec![Arc::new(BooleanArray::from(vec![
                    Some(true),
                    None,
                    Some(false),
                ]))],
            )
            .unwrap(),
            schema,
        )
        .unwrap();
        let mut arena = ExprArena::default();
        let flag = arena.push_typed(ExprNode::SlotId(slot), DataType::Boolean);
        let inverse = arena.push_typed(ExprNode::Not(flag), DataType::Boolean);
        (arena, vec![inverse, flag], input)
    }

    #[test]
    fn actual_split_remaining_has_its_own_creator_and_original_null_routing() {
        let (arena, exprs, input) = typed_split_fixture();
        let state = RuntimeState::default();
        let factory =
            crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                crate::runtime::mem_tracker::MemTracker::new_root("split actual creator fixture"),
                state.error_state(),
            )
            .unwrap();
        let control = crate::exec::operators::compiled_expression::RuntimeKernelControl::new(
            state.error_state(),
        );
        let mut evaluations = Vec::new();
        let mut filters = Vec::new();
        let out = super::split_chunk_by_exprs_with(
            &exprs,
            input,
            false,
            &mut |expr, input| {
                evaluations.push((
                    expr,
                    input.len(),
                    input.construction_storage_lifetime().is_pure(),
                ));
                let owner = factory.begin_input_construction(input)?;
                crate::exec::expr::ExprEvaluationContext::new(&arena, owner.as_ref(), &control)
                    .eval(expr, input)
            },
            &mut |input, mask| {
                filters.push(input.len());
                let owner = factory.begin_input_construction(input)?;
                input.filter_for_evaluation(mask, owner.as_ref())
            },
        )
        .unwrap();
        assert_eq!(evaluations, vec![(exprs[1], 3, true), (exprs[0], 2, false)]);
        assert_eq!(filters, vec![3, 3, 2]);
        for (index, expected) in [(0, false), (1, true)] {
            let output = out[index].as_ref().unwrap();
            assert_eq!(output.len(), 1);
            assert!(!output.construction_storage_lifetime().is_pure());
            assert_eq!(
                output.columns()[0]
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(expected)]
            );
        }
    }

    #[test]
    fn split_filter_and_next_evaluation_preserve_the_original_runtime_failure() {
        use crate::runtime::fragment::{ExecutionFailure, PipelineOperation};
        use novarocks_functions::{KernelDiagnostic, KernelFailure};
        let mut failures: Vec<ExecutionFailure> = vec![
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InstanceFailed,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("exact invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("exact internal")),
            KernelFailure::Operational(KernelDiagnostic::new("exact operation")),
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        failures.push(ExecutionFailure::from("cancelled").at_operator(47, PipelineOperation::Push));
        for original in failures {
            let (arena, exprs, input) = typed_split_fixture();
            let state = RuntimeState::default();
            let factory =
                crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                    crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                    crate::runtime::mem_tracker::MemTracker::new_root("split failure fixture"),
                    state.error_state(),
                )
                .unwrap();
            let control = crate::exec::operators::compiled_expression::RuntimeKernelControl::new(
                state.error_state(),
            );
            let mut calls = 0;
            let failure = super::split_chunk_by_exprs_with(
                &exprs,
                input,
                false,
                &mut |expr, input| {
                    calls += 1;
                    assert_eq!(calls, 1, "no evaluation after originating failure");
                    let owner = factory.begin_input_construction(input)?;
                    let result = crate::exec::expr::ExprEvaluationContext::new(
                        &arena,
                        owner.as_ref(),
                        &control,
                    )
                    .eval(expr, input)?;
                    state.error_state().set_error(original.clone());
                    Ok(result)
                },
                &mut |input, mask| {
                    let owner = factory.begin_input_construction(input)?;
                    input.filter_for_evaluation(mask, owner.as_ref())
                },
            )
            .unwrap_err();
            assert_eq!(failure, original);
            assert_eq!(calls, 1);
        }
    }
}
