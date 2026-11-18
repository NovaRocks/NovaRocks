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

//! Original Native tail and joined producer-cut custody on credentialed work.
//! Future return, actual Join, Native settlement and metadata retirement remain
//! independent; no logical Core action or synthetic exit is authored here.
use super::{
    auxiliary_work::{AuxiliaryHeaderOwner, AuxiliaryWorkObligation, AuxiliaryWorkStart},
    failure::LogicalInfrastructureFailure,
    funding::LogicalFundingObligation,
};
use crate::control_runtime::actor::FrontendActor;
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime},
    exit_custody::ExitCustody,
    reported_work::ReportedWork,
};
use novarocks_control_runtime_core::scope::WorkKind;
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_query_application::coordination::{
    StreamedRowsComputedCut,
    executor::{
        NativeExecutor, OriginalTailJoinObligation, OriginalTailKind, OriginalTailWorkObligation,
    },
};
use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
use tokio::runtime::Handle;

struct TailRow {
    run: Option<ReportedWork>,
    settle: Option<ReportedWork>,
    job: Option<Arc<AuxiliaryWorkObligation>>,
    settlement: Option<Arc<AuxiliaryWorkObligation>>,
    joined: Arc<Mutex<Option<OriginalTailJoinObligation>>>,
    claimed: bool,
    join_transferred: bool,
    job_retired: bool,
    settlement_retired: bool,
    complete: bool,
}
struct CutRow {
    original: Option<ReportedWork>,
    job: Option<Arc<AuxiliaryWorkObligation>>,
    claimed: bool,
    complete: bool,
}
pub(super) struct TailProgress {
    pub(super) units: usize,
    pub(super) progressed: bool,
    pub(super) more: bool,
    pub(super) retired: bool,
}
/// One original executor and fixed A*2 tail positions. This Source is moved to
/// controlled final disposal; dropping its last Native Arc never runs on a poller.
pub(super) struct LogicalTailSource {
    native: Option<Arc<NativeExecutor>>,
    rows: Vec<TailRow>,
    cuts: Vec<CutRow>,
    cursor: usize,
    remaining: usize,
    queue_done: bool,
    closed: bool,
    live_rows: usize,
    runtime: ActorRuntime<FrontendActor>,
    actor: ActorRegistration,
    handle: Handle,
    wake: Waker,
    failure: Arc<LogicalInfrastructureFailure>,
}
impl LogicalTailSource {
    pub(super) fn reserve(
        native: Option<Arc<NativeExecutor>>,
        funding: &LogicalFundingObligation,
        attempts: usize,
        runtime: ActorRuntime<FrontendActor>,
        actor: ActorRegistration,
        handle: Handle,
        wake: Waker,
        failure: Arc<LogicalInfrastructureFailure>,
    ) -> Self {
        // These already-admitted cleanup roles survive an owner fence. No
        // failed Scope must issue fresh work to settle an earlier real Join.
        let reserve = || {
            funding
                .reserve_reported(WorkKind::Cleanup, 0)
                .expect("original prepaid tail custody work")
        };
        let rows = (0..attempts * 2)
            .map(|_| TailRow {
                run: Some(reserve()),
                settle: Some(reserve()),
                job: None,
                settlement: None,
                joined: Arc::new(Mutex::new(None)),
                claimed: false,
                join_transferred: false,
                job_retired: false,
                settlement_retired: false,
                complete: false,
            })
            .collect();
        let cuts = (0..attempts)
            .map(|_| CutRow {
                original: Some(reserve()),
                job: None,
                claimed: false,
                complete: false,
            })
            .collect();
        Self {
            native,
            rows,
            cuts,
            cursor: 0,
            remaining: 0,
            queue_done: false,
            closed: false,
            live_rows: attempts * 3,
            runtime,
            actor,
            handle,
            wake,
            failure,
        }
    }
    pub(super) fn begin_pass(&mut self) {
        self.remaining = self.rows.len() + self.cuts.len();
        self.queue_done = false;
    }
    pub(super) fn close(&mut self) {
        self.closed = true;
    }
    fn start(
        &self,
        original: ReportedWork,
        make: super::auxiliary_work::AuxiliaryFactory,
    ) -> Arc<AuxiliaryWorkObligation> {
        AuxiliaryWorkObligation::start(AuxiliaryWorkStart {
            handle: self.handle.clone(),
            original,
            make,
            wake: self.wake.clone(),
            failure: self.failure.reporter(),
            retirement: AuxiliaryHeaderOwner {
                runtime: self.runtime.clone(),
                actor: self.actor.clone(),
            },
        })
    }
    pub(super) fn stage_cut(&mut self, index: usize, cut: StreamedRowsComputedCut) {
        let Some(row) = self
            .cuts
            .get_mut(index)
            .filter(|r| !r.claimed && !r.complete)
        else {
            self.failure.retain(Box::new(cut));
            return;
        };
        row.claimed = true;
        let original = row
            .original
            .take()
            .expect("original joined producer-cut cleanup role");
        let mut cut = Some(cut);
        let failure = self.failure.clone();
        let job = self.start(
            original,
            Box::new(move || {
                let cut = cut.take().expect("same original joined producer cut");
                let failure = failure.clone();
                Box::pin(async move {
                    if let Err(original) = cut.retire_after_pump_exit().await {
                        failure.retain(Box::new(original));
                    }
                })
            }),
        );
        self.cuts[index].job = Some(job);
    }
    fn stage_tail(&mut self, index: usize, original: OriginalTailWorkObligation) {
        let offset = match original.kind() {
            OriginalTailKind::RootAck => 0,
            OriginalTailKind::Consumer => 1,
        };
        let slot = index * 2 + offset;
        let Some(row) = self
            .rows
            .get_mut(slot)
            .filter(|r| !r.claimed && !r.complete)
        else {
            self.failure.retain(Box::new(original));
            return;
        };
        row.claimed = true;
        let prepaid = row.run.take().expect("original prepaid Native tail work");
        let joined = row.joined.clone();
        let mut original = Some(original);
        let job = self.start(
            prepaid,
            Box::new(move || {
                // The actual factory and input transfer occur only under the
                // original credential. Source first owns the stable launch slot.
                let (future, obligation) = original
                    .take()
                    .expect("same original Native tail")
                    .into_future();
                assert!(joined.lock().unwrap().replace(obligation).is_none());
                future
            }),
        );
        self.rows[slot].job = Some(job);
    }
    pub(super) fn progress(
        &mut self,
        cx: &mut Context<'_>,
        budget: usize,
        mut activation_index: impl FnMut(AttemptActivationIdentity) -> Option<usize>,
    ) -> TailProgress {
        let mut units = 0;
        let mut progressed = false;
        if !self.queue_done && units < budget {
            units += 1;
            if let Some(native) = &self.native {
                match native.poll_tail_work(cx) {
                    Poll::Pending => self.queue_done = true,
                    Poll::Ready(original) => {
                        let activation = original.activation();
                        if let Some(index) = activation_index(activation) {
                            self.stage_tail(index, original);
                        } else {
                            self.failure.retain(Box::new(original));
                        }
                        progressed = true;
                    }
                }
            } else {
                self.queue_done = true;
            }
        }
        let visits = self.remaining.min(budget.saturating_sub(units));
        for _ in 0..visits {
            let index = self.cursor;
            self.cursor = (self.cursor + 1) % (self.rows.len() + self.cuts.len());
            self.remaining -= 1;
            units += 1;
            if index < self.rows.len() {
                let before = self.rows[index].complete;
                progressed |= self.progress_tail(index, cx);
                if !before && self.rows[index].complete {
                    self.live_rows -= 1;
                }
            } else {
                let index = index - self.rows.len();
                let before = self.cuts[index].complete;
                progressed |= self.progress_cut(index, cx);
                if !before && self.cuts[index].complete {
                    self.live_rows -= 1;
                }
            }
        }
        TailProgress {
            units,
            progressed,
            more: !self.queue_done || self.remaining != 0,
            retired: self.closed && self.live_rows == 0,
        }
    }
    fn progress_tail(&mut self, index: usize, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        let row = &mut self.rows[index];
        if row.complete {
            return false;
        }
        if let Some(job) = &row.job {
            match job.poll_complete(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(_)) | Poll::Pending => {}
            }
            row.job_retired = job.retired();
            if !row.join_transferred
                && let Some(outcome) = job.take_join_outcome()
            {
                let obligation = row.joined.lock().unwrap().take();
                if let Some(obligation) = obligation {
                    row.join_transferred = true;
                    let native = self
                        .native
                        .as_ref()
                        .expect("tail came from its original Native executor")
                        .clone();
                    let failure = self.failure.clone();
                    let original = row
                        .settle
                        .take()
                        .expect("original prepaid Native Join settlement");
                    let mut packet = Some((native, obligation, outcome));
                    let settlement = self.start(
                        original,
                        Box::new(move || {
                            let (native, obligation, outcome) =
                                packet.take().expect("same original tail Join packet");
                            let failure = failure.clone();
                            Box::pin(async move {
                                if let Err(original) = native.tail_joined(obligation, outcome) {
                                    failure.retain_typed(original);
                                }
                                if native.has_unhandled_failure()
                                    && let Some(original) = native.failure_source()
                                {
                                    failure.retain(Box::new(original));
                                }
                            })
                        }),
                    );
                    self.rows[index].settlement = Some(settlement);
                    progressed = true;
                } else {
                    // A never-started or failed factory keeps its actual input
                    // and unique obligation in original auxiliary custody.
                    self.failure.retain(Box::new(
                        "Native tail Join has no returned original obligation",
                    ));
                }
            }
        }
        let row = &mut self.rows[index];
        if let Some(settlement) = &row.settlement {
            let _ = settlement.poll_complete(cx);
            row.settlement_retired = settlement.retired();
        }
        if row.job_retired && row.settlement_retired {
            row.job = None;
            row.settlement = None;
            row.complete = true;
            return true;
        }
        if self.closed && self.queue_done && !row.claimed {
            if let Some(original) = row.run.take() {
                retire_unused(original);
                progressed = true;
            }
            if let Some(original) = row.settle.take() {
                retire_unused(original);
                progressed = true;
            }
            row.complete = true;
        }
        progressed
    }
    fn progress_cut(&mut self, index: usize, cx: &mut Context<'_>) -> bool {
        let row = &mut self.cuts[index];
        if row.complete {
            return false;
        }
        if let Some(job) = &row.job {
            let _ = job.poll_complete(cx);
            if job.retired() {
                row.job = None;
                row.complete = true;
                return true;
            }
        }
        if self.closed && self.queue_done && !row.claimed {
            if let Some(original) = row.original.take() {
                retire_unused(original);
            }
            row.complete = true;
            return true;
        }
        false
    }
}
fn retire_unused(original: ReportedWork) {
    let scope = original.work().scope().clone();
    let header = original.work().retirement_observer();
    let (custody, exit) = ExitCustody::from_admitted(original);
    exit.revoke_unused_and_exit()
        .expect("unused original Source auxiliary role");
    assert!(
        custody
            .try_retire()
            .expect("original unused auxiliary custody")
    );
    assert!(
        scope
            .reap_observed_header(&header)
            .expect("original unused auxiliary header")
    );
}
