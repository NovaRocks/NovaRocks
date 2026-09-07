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
//! Multi-cast stream sink for replicated exchange output.
//!
//! Responsibilities:
//! - Sends the same serialized chunk stream to multiple remote destinations.
//! - Coordinates per-destination transport state and completion signaling.
//!
//! Key exported interfaces:
//! - Types: `MultiCastDataStreamSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use crate::runtime::fragment::io::exchange_edge::ExchangeEdgeGates;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
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
    limit_remaining: Option<Arc<AtomicI64>>,
    factory: DataStreamSinkFactory,
}

/// Factory for multi-cast stream sinks that replicate output to multiple remote channels.
pub struct MultiCastDataStreamSinkFactory {
    name: String,
    init_error: Option<String>,
    sinks: Vec<InnerSinkSpec>,
}

impl MultiCastDataStreamSinkFactory {
    pub fn new(
        sinks: Vec<(DataStreamSinkFactoryInput, Option<i64>)>,
        fragment_instance_id: UniqueId,
        sender_id: Option<i32>,
        partition_arena: ExprArena,
        plan_node_id: i32,
        transmitter: Arc<dyn ExchangeFrameTransmitter>,
    ) -> Self {
        let name = if plan_node_id >= 0 {
            format!("MULTI_CAST_DATA_STREAM_SINK (id={plan_node_id})")
        } else {
            "MULTI_CAST_DATA_STREAM_SINK".to_string()
        };
        let mut init_error = None;
        let mut out = Vec::new();

        if sinks.is_empty() {
            init_error = Some("MULTI_CAST_DATA_STREAM_SINK requires at least one sink".to_string());
        } else {
            for (sink, limit) in sinks {
                let limit_remaining = match limit {
                    Some(v) if v != -1 && v >= 0 => Some(Arc::new(AtomicI64::new(v))),
                    _ => None,
                };

                out.push(InnerSinkSpec {
                    limit_remaining,
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
            sinks: out,
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

impl OperatorFactory for MultiCastDataStreamSinkFactory {
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
                limit_remaining: spec.limit_remaining.clone(),
                op: spec.factory.create(dop, driver_id),
            });
        }

        Box::new(MultiCastDataStreamSinkOperator::new(
            self.name.clone(),
            self.init_error.clone(),
            sinks,
        ))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct InnerSinkRuntime {
    limit_remaining: Option<Arc<AtomicI64>>,
    op: Box<dyn Operator>,
}

struct MultiCastDataStreamSinkOperator {
    name: String,
    init_error: Option<String>,
    sinks: Vec<InnerSinkRuntime>,
    finishing: bool,
    sink_observable: Arc<Observable>,
}

impl MultiCastDataStreamSinkOperator {
    fn new(name: String, init_error: Option<String>, sinks: Vec<InnerSinkRuntime>) -> Self {
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
            sinks,
            finishing: false,
            sink_observable,
        }
    }
}

impl Operator for MultiCastDataStreamSinkOperator {
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

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        for sink in &mut self.sinks {
            sink.op.bind_runtime_state(state)?;
        }
        Ok(())
    }

    fn prepare(&mut self) -> Result<(), String> {
        for sink in &mut self.sinks {
            sink.op.prepare()?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
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

impl ProcessorOperator for MultiCastDataStreamSinkOperator {
    /// The aggregate of what this sink's branches are still waiting for.
    ///
    /// Forwarding this is what earns the composite another `set_finishing`
    /// turn. Without it the default `Complete` applies, the driver latches
    /// finishing after one call, and any branch that was not ready at that
    /// call -- a closed exchange edge, a parked payload -- never sends its
    /// end-of-stream.
    ///
    /// `OwedOutput` outranks `ExternalEvent` here, which is the opposite of
    /// one branch's own order, and the difference is the point: branches
    /// finish independently, so a branch holding output is moved by a turn now
    /// even while a *different* branch waits for its edge to open. Reporting
    /// the external event would park the driver on the branch that cannot
    /// progress and strand the one that can.
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

    // Design: ADR-0002 (docs/adr/ADR-0002-multicast-coupled-backpressure.md)
    fn need_input(&self) -> bool {
        if self.is_finished() || self.finishing {
            return false;
        }
        for sink in &self.sinks {
            let allowed = sink
                .limit_remaining
                .as_ref()
                .map(|remaining| remaining.load(Ordering::SeqCst) > 0)
                .unwrap_or(true);
            if !allowed {
                continue;
            }
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

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if let Some(err) = self.init_error.as_ref() {
            return Err(err.clone());
        }
        if self.is_finished() || self.finishing {
            return Ok(());
        }
        if chunk.is_empty() || self.sinks.is_empty() {
            return Ok(());
        }

        let input_chunk = chunk;
        let mut should_send = Vec::with_capacity(self.sinks.len());
        for sink in &mut self.sinks {
            let allowed = sink
                .limit_remaining
                .as_ref()
                .map(|remaining| remaining.load(Ordering::SeqCst) > 0)
                .unwrap_or(true);
            should_send.push(allowed);
        }

        let mut per_sink_chunks = Vec::with_capacity(self.sinks.len());
        for (sink, allowed) in self.sinks.iter_mut().zip(should_send.iter().copied()) {
            if !allowed {
                per_sink_chunks.push(None);
                continue;
            }
            let limited = apply_limit(&input_chunk, sink.limit_remaining.as_ref())
                .map_err(|e| e.to_string())?;
            per_sink_chunks.push(limited);
        }

        for (sink, chunk) in self.sinks.iter_mut().zip(per_sink_chunks.into_iter()) {
            let Some(chunk) = chunk else {
                continue;
            };
            let inner = sink
                .op
                .as_processor_mut()
                .ok_or_else(|| "inner data stream op missing processor operator".to_string())?;
            inner.push_chunk(state, chunk)?;
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    /// Drives every branch's finishing, on this call and on every retry.
    ///
    /// A branch does not necessarily finish on its first `set_finishing`: an
    /// exchange sink whose edge is still closed, or which still holds a parked
    /// payload, latches its last-driver verdict and waits for the turn the
    /// driver gives it while [`Self::finishing_wait`] reports pending.
    /// Returning early once `finishing` was set swallowed those turns, so a
    /// branch that was not ready at the first call never sent its
    /// end-of-stream and its receiver counted a sender that never sealed.
    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String> {
        if let Some(err) = self.init_error.as_ref() {
            return Err(err.clone());
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

fn apply_limit(chunk: &Chunk, remaining: Option<&Arc<AtomicI64>>) -> Result<Option<Chunk>, String> {
    let Some(remaining) = remaining else {
        return Ok(Some(chunk.clone()));
    };

    let rows = chunk.len() as i64;
    if rows <= 0 {
        return Ok(None);
    }

    loop {
        let cur = remaining.load(Ordering::SeqCst);
        if cur <= 0 {
            return Ok(None);
        }
        let send = rows.min(cur);
        if remaining
            .compare_exchange(cur, cur - send, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let send = send as usize;
            if send == 0 {
                return Ok(None);
            }
            if send == chunk.len() {
                return Ok(Some(chunk.clone()));
            }
            return Ok(Some(chunk.slice(0, send)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct BindingSink {
        binds: Arc<AtomicUsize>,
    }

    impl Operator for BindingSink {
        fn name(&self) -> &str {
            "binding"
        }

        fn bind_runtime_state(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.binds.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

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

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.finishing = true;
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            (!self.is_finished()).then(|| Arc::clone(&self.observable))
        }
    }

    /// Inner sink whose send queue is permanently full.
    struct RefusingSink {
        name: String,
    }

    impl Operator for RefusingSink {
        fn name(&self) -> &str {
            &self.name
        }

        fn is_finished(&self) -> bool {
            false
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for RefusingSink {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Err("refusing sink must not receive input".to_string())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            None
        }
    }

    #[test]
    fn multi_cast_sink_waits_for_inner_sinks_to_finish() {
        let first_done = Arc::new(AtomicBool::new(false));
        let second_done = Arc::new(AtomicBool::new(false));
        let mut op = MultiCastDataStreamSinkOperator::new(
            "MULTI_CAST_DATA_STREAM_SINK(test)".to_string(),
            None,
            vec![
                InnerSinkRuntime {
                    limit_remaining: None,
                    op: Box::new(PendingFinishSink::new("first", Arc::clone(&first_done))),
                },
                InnerSinkRuntime {
                    limit_remaining: None,
                    op: Box::new(PendingFinishSink::new("second", Arc::clone(&second_done))),
                },
            ],
        );

        let state = RuntimeState::default();
        assert!(!op.is_finished());

        op.set_finishing(&state).expect("set finishing");
        assert!(
            !op.is_finished(),
            "wrapper must wait for async inner stream sinks"
        );
        assert!(
            op.sink_observable().is_some(),
            "wrapper must keep an observable while inner sinks drain"
        );

        first_done.store(true, Ordering::SeqCst);
        assert!(!op.is_finished(), "wrapper must wait for every inner sink");

        second_done.store(true, Ordering::SeqCst);
        assert!(op.is_finished());
        assert!(op.sink_observable().is_none());
    }

    #[test]
    fn multicast_uses_one_identity_and_forwards_non_first_inner_wakes() {
        let first = PendingFinishSink::new("first", Arc::new(AtomicBool::new(false)));
        let second = PendingFinishSink::new("second", Arc::new(AtomicBool::new(false)));
        let second_observable = Arc::clone(&second.observable);
        let op = MultiCastDataStreamSinkOperator::new(
            "MULTI_CAST_DATA_STREAM_SINK(test)".to_string(),
            None,
            vec![
                InnerSinkRuntime {
                    limit_remaining: None,
                    op: Box::new(first),
                },
                InnerSinkRuntime {
                    limit_remaining: None,
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
    fn one_full_branch_blocks_the_whole_multicast_sink() {
        // One accepting inner and one refusing inner: the multicast operator
        // must stop accepting input entirely. This is the execution fact
        // behind the deployment-side backpressure edges.
        let accepting_done = Arc::new(AtomicBool::new(false));
        let op = MultiCastDataStreamSinkOperator::new(
            "MULTI_CAST_DATA_STREAM_SINK(test)".to_string(),
            None,
            vec![
                InnerSinkRuntime {
                    limit_remaining: None,
                    op: Box::new(PendingFinishSink::new("accepting", accepting_done)),
                },
                InnerSinkRuntime {
                    limit_remaining: None,
                    op: Box::new(RefusingSink {
                        name: "refusing".to_string(),
                    }),
                },
            ],
        );

        assert!(!op.need_input());
    }

    #[test]
    fn multicast_binds_runtime_state_to_every_inner_sink() {
        let binds = Arc::new(AtomicUsize::new(0));
        let mut op = MultiCastDataStreamSinkOperator::new(
            "MULTI_CAST_DATA_STREAM_SINK(test)".to_string(),
            None,
            vec![
                InnerSinkRuntime {
                    limit_remaining: None,
                    op: Box::new(BindingSink {
                        binds: Arc::clone(&binds),
                    }),
                },
                InnerSinkRuntime {
                    limit_remaining: None,
                    op: Box::new(BindingSink {
                        binds: Arc::clone(&binds),
                    }),
                },
            ],
        );

        op.bind_runtime_state(&RuntimeState::default())
            .expect("bind inner sinks");
        assert_eq!(binds.load(Ordering::SeqCst), 2);
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

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
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
        // The defect this catches: the wrapper reported no finishing wait and
        // refused to re-enter its branches, so the driver latched finishing
        // after one call. A branch whose exchange edge was still closed at
        // that call never sent its end-of-stream, and its receiver counted a
        // sender that never sealed -- the query hung until the statement
        // timeout. Every multi-cast query whose edges do not all open before
        // its producer drains is affected.
        let open = Arc::new(AtomicBool::new(true));
        let closed = Arc::new(AtomicBool::new(false));
        let open_sealed = Arc::new(AtomicBool::new(false));
        let closed_sealed = Arc::new(AtomicBool::new(false));
        let mut op = MultiCastDataStreamSinkOperator::new(
            "MULTI_CAST_DATA_STREAM_SINK(test)".to_string(),
            None,
            vec![
                InnerSinkRuntime {
                    limit_remaining: None,
                    op: Box::new(GatedFinishSink::new(
                        "open",
                        Arc::clone(&open),
                        Arc::clone(&open_sealed),
                    )),
                },
                InnerSinkRuntime {
                    limit_remaining: None,
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
        assert!(
            open_sealed.load(Ordering::SeqCst),
            "a branch that already had permission seals on the first call"
        );
        assert!(!closed_sealed.load(Ordering::SeqCst));
        assert!(
            op.finishing_wait().is_pending(),
            "the wrapper must ask the driver for another turn while a branch waits"
        );
        assert!(!op.is_finished());

        // The frontend opens the second branch's edge.
        closed.store(true, Ordering::SeqCst);
        op.set_finishing(&state).expect("set finishing again");
        assert!(
            closed_sealed.load(Ordering::SeqCst),
            "the retry must reach the branch that was not ready before"
        );
        assert!(!op.finishing_wait().is_pending());
        assert!(op.is_finished());
    }
}
