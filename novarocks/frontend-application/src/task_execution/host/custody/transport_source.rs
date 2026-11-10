// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The attempt Source visits its actual admitted transport rows in bounded
//! passes. A wake marks the original host incarnation, never a replacement.

use super::{
    TransportRetirement, TransportRetirementError, TransportRetirementStep,
    actor_result::ReturnedActorTransportResult,
};
use crate::{
    control_runtime::{actor::FrontendActor, transport_withdrawal::AttemptEffectEnvelope},
    native::apply_send_owner::RegistrationFailure,
    task_execution::host::transport::SendReportReturn,
};
use novarocks_control_runtime::actor_turn::{ActorHostTurn, ActorRegistration, ActorRuntime};
use novarocks_coordination_contract::AttemptActionId;
use std::{
    alloc::Layout,
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
use tokio::runtime::Handle;

struct HostWake {
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    marked: AtomicBool,
}
impl Wake for HostWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.marked.store(true, Ordering::Release);
        // A late callback can race the actual actor removal. The runtime
        // qualifies the original registration and cannot wake its successor.
        let _ = self.runtime.notify_host_actor(&self.registration);
    }
}

#[must_use = "retain original results and rejected proofs until their owner consumes them"]
pub(crate) enum TransportSourceEvent {
    Result(ReturnedActorTransportResult),
    Report(SendReportReturn),
    RejectedAttachment(RegistrationFailure),
    RefusedResult(ReturnedActorTransportResult),
    Failed(TransportRetirementError),
    Retired,
}
pub(crate) struct TransportSourceProgress {
    pub(crate) units: usize,
    pub(crate) progressed: bool,
    pub(crate) more: bool,
}

struct Row {
    effect: Arc<AttemptEffectEnvelope>,
    retirement: Box<TransportRetirement>,
}

/// The Source preprices the number of resident rows with the attempt's
/// original result-role inventory. A cursor inspection costs one host unit.
/// The rows and their original Work stay here even on failed cleanup.
pub(crate) struct TransportRetirements {
    rows: VecDeque<Row>,
    capacity: usize,
    remaining: usize,
    rescan: bool,
    wake: Arc<HostWake>,
    waker: Waker,
    executor: Handle,
}

impl TransportRetirements {
    pub(crate) fn layouts(
        rows: usize,
    ) -> Result<[Layout; 2], novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    {
        use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
        Ok([
            Layout::array::<Row>(rows).map_err(|_| LayoutResourceError::Arithmetic)?,
            arc_layout(Layout::new::<HostWake>())?,
        ])
    }

    pub(crate) fn new(
        runtime: ActorRuntime<FrontendActor>,
        registration: ActorRegistration,
        executor: Handle,
        capacity: usize,
    ) -> Self {
        let wake = Arc::new(HostWake {
            runtime,
            registration,
            marked: AtomicBool::new(false),
        });
        let waker = Waker::from(wake.clone());
        Self {
            rows: VecDeque::with_capacity(capacity),
            capacity,
            remaining: 0,
            rescan: false,
            wake,
            waker,
            executor,
        }
    }

    pub(crate) fn has_room(&self) -> bool {
        self.rows.len() < self.capacity
    }

    pub(crate) fn matches_registration(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        registration: &ActorRegistration,
    ) -> bool {
        self.wake.runtime.same_runtime(runtime)
            && self.wake.registration.same_registration(registration)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Install while binding the already-prepaid result, before handoff can
    /// run. If an earlier pass has visited all the older rows, this mark
    /// guarantees a later pass over the newly installed row as well.
    pub(crate) fn track(
        &mut self,
        effect: Arc<AttemptEffectEnvelope>,
        retirement: Box<TransportRetirement>,
    ) -> Result<(), (Arc<AttemptEffectEnvelope>, Box<TransportRetirement>)> {
        if !self.has_room()
            || !effect.uses_join_custody()
            || !self.wake.registration.matches_scope(retirement.scope())
        {
            return Err((effect, retirement));
        }
        self.rows.push_back(Row { effect, retirement });
        self.wake.wake_by_ref();
        Ok(())
    }

    /// Core input consumption and effect completion can unblock a report
    /// position that this pass already visited.
    pub(crate) fn original_input_progress(&self) {
        if !self.rows.is_empty() {
            self.wake.wake_by_ref();
        }
    }

    pub(crate) fn progress(
        &mut self,
        turn: &ActorHostTurn<FrontendActor>,
        budget: usize,
        mut receive: impl FnMut(AttemptActionId, TransportSourceEvent),
    ) -> TransportSourceProgress {
        assert!(turn.matches_registration(&self.wake.registration));
        let woke = self.wake.marked.swap(false, Ordering::AcqRel);
        if self.remaining == 0 {
            self.remaining = self.rows.len();
            self.rescan = false;
        } else {
            self.rescan |= woke;
        }
        let mut progressed = false;
        let units = self.remaining.min(budget);
        let mut cx = Context::from_waker(&self.waker);
        for _ in 0..units {
            let mut row = self.rows.pop_front().expect("original scan inventory");
            self.remaining -= 1;
            let (changed, event) = row.advance(&self.executor, &mut cx);
            let id = row.effect.protocol.id;
            let retired = matches!(event, Some(TransportSourceEvent::Retired));
            if !retired {
                self.rows.push_back(row);
            }
            if let Some(event) = event {
                receive(id, event);
            }
            progressed |= changed;
        }
        self.rescan |= progressed;
        TransportSourceProgress {
            units,
            progressed,
            more: self.remaining != 0 || self.rescan && !self.rows.is_empty(),
        }
    }
}

impl Row {
    fn advance(
        &mut self,
        executor: &Handle,
        cx: &mut Context<'_>,
    ) -> (bool, Option<TransportSourceEvent>) {
        use TransportSourceEvent as Event;
        match self.retirement.take_actor_result() {
            Ok(Some(result)) if Arc::ptr_eq(&result.effect, &self.effect) => {
                return (true, Some(Event::Result(result)));
            }
            Ok(Some(result)) | Err(result) => {
                self.retirement.retain_unresolved();
                return (true, Some(Event::RefusedResult(result)));
            }
            Ok(None) => {}
        }
        if let Some(refused) = self.retirement.take_rejected_attachment() {
            return (true, Some(Event::RejectedAttachment(refused)));
        }
        match self.retirement.poll_step(executor, cx) {
            Poll::Pending => (false, None),
            Poll::Ready(Ok(TransportRetirementStep::Scanned)) => {
                // One internal report pass can need another bounded visit,
                // even though this visit did not release an original.
                cx.waker().wake_by_ref();
                (false, None)
            }
            Poll::Ready(Ok(TransportRetirementStep::Advanced)) => (true, None),
            Poll::Ready(Ok(TransportRetirementStep::Report(report))) => {
                (true, Some(Event::Report(report)))
            }
            Poll::Ready(Ok(TransportRetirementStep::Retired)) => (true, Some(Event::Retired)),
            Poll::Ready(Err(error)) => (true, Some(Event::Failed(error))),
        }
    }
}
